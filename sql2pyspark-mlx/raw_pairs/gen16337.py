from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("opened_date").desc_nulls_last(), "id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-07-23").cast("date")) & (F.col("sale_date") < "2023-09-23"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id")
)

joined = (
    ranked
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.opened_date"), F.col("b.sqft"), F.col("b.store_name"))
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

remaining_keys = (
    deduped
    .select("id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("units").between(16, 24))
        .select("id", "store_id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("src")
    .join(df_stores.alias("dim"), F.col("src.store_id") == F.col("dim.id"), "left")
    .cube(F.col("dim.region").alias("region"), F.col("dim.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.sqft")).alias("total_sqft"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("city").asc_nulls_first())
)
