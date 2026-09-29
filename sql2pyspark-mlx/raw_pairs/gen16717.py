from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w_ordered = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("last_restock").asc_nulls_last(), "id")
w_part = Window.partitionBy("store_id", "product_id").orderBy("row_num", "id").rowsBetween(-6, Window.currentRow)
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

latest = (
    df_inventory
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered = (
    latest
    .filter(F.col("quantity") < 197)
    .select("*", F.round("quantity", 2).alias("quantity_rounded"))
)

metrics = (
    filtered
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.sum("quantity_rounded").over(w_ordered).alias("last4_sum_quantity_rounded"),
        F.max("quantity_rounded").over(w_ordered).alias("last4_max_quantity_rounded"),
        F.avg("quantity_rounded").over(w_ordered).alias("last4_avg_quantity_rounded"),
    )
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .filter(F.col("product_id").isin([2, 4, 6]))
    .select(
        "*",
        F.count("last4_sum_quantity_rounded").over(w_part).alias("last7_count_last4_sum_quantity_rounded"),
        F.sum("last4_sum_quantity_rounded").over(w_part).alias("last7_sum_last4_sum_quantity_rounded"),
        F.min("last4_sum_quantity_rounded").over(w_part).alias("last7_min_last4_sum_quantity_rounded"),
    )
)

result = (
    scored
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last4_sum_quantity_rounded",
        "last4_max_quantity_rounded",
        "last4_avg_quantity_rounded",
        "row_num",
        F.col("avg_store_sale_revenue"),
    )
    .orderBy(F.col("row_num").desc())
)
