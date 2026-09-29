from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("running_avg_revenue_per_unit").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc())

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-10").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.avg("revenue_per_unit").over(w_ordered).alias("running_avg_revenue_per_unit"),
        F.min("revenue_per_unit").over(w_ordered).alias("running_min_revenue_per_unit"),
        F.count("revenue_per_unit").over(w_ordered).alias("running_count_revenue_per_unit"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    deduped
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.sale_date",
        "e.running_min_revenue_per_unit",
        "e.running_count_revenue_per_unit",
        "e.row_num",
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
)

deduped_store_sales = (
    with_dims
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "store_name")
)

result = (
    deduped_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("num_rows_2"),
    )
)
