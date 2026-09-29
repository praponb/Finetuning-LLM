from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region", "city").orderBy(F.col("opened_date").desc_nulls_last())

cleaned = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-08-13").cast("date"))
    .select(
        "*",
        F.when(F.col("region") == "West", 1).otherwise(0).alias("is_west"),
        F.concat_ws("/", "city", "region").alias("city_region_key"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"), F.col("c.units"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.revenue").between(2400, 2700)), "left_anti")
)

cleaned_stores = (
    kept
    .filter(F.col("is_west").between(41, 53))
    .select(
        "*",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.ceil("store_sale_revenue").alias("store_sale_revenue_whole"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region_2"),
    )
)

deduped = (
    cleaned_stores
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "store_name",
        "opened_date",
        "is_west",
        "resolved_region",
        "store_name_normalized",
        "store_sale_revenue_whole",
        "resolved_region_2",
    )
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "resolved_region",
        "store_name_normalized",
        "store_sale_revenue_whole",
        "resolved_region_2",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Old Town)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("store_name_zone"),
    )
)
