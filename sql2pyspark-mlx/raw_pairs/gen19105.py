from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("opened_date_dayofweek", F.dayofweek("opened_date"))
    .withColumn("region_filled", F.coalesce("region", F.lit("DIRECT")))
)

base = (
    cleaned
    .filter((F.col("opened_date") > F.lit("2023-05-11").cast("date")) & F.col("sqft").between(5112, 11670))
    .withColumn("city_length", F.length("city"))
    .withColumn("city_prefix", F.substring("city", 1, 3))
)

kept = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") < "2025-04-01"), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.opened_date")), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"), F.col("c.revenue"))
)

result = (
    with_latest
    .select(
        "id",
        "city",
        "region",
        "opened_date_dayofweek",
        "city_length",
        "city_prefix",
        "revenue",
        F.when(F.col("region").rlike("^(West|East)$"), "OTHER_ZONE").when(F.col("region").rlike("^(North|South)$"), "CORE").otherwise("GROUP_A").alias("region_zone"),
    )
)
