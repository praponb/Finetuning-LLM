from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
discount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("discount").alias("sum_order_discount"))

valid = (
    df_inventory
    .filter(F.col("quantity").between(115, 203))
    .withColumn("quantity_whole", F.ceil("quantity"))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

base = (
    valid
    .filter(F.col("last_restock") > F.lit("2023-05-04").cast("date"))
    .select("*", F.ceil("quantity_whole").alias("quantity_whole_whole"))
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped_inventory = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

report = (
    deduped_inventory
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_whole",
        "quantity_whole_whole",
        "latest_rank",
        F.col("sum_order_discount"),
    )
)

result = (
    report
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_whole_whole",
        "latest_rank",
        "sum_order_discount",
    )
)
