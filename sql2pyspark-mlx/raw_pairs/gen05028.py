from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("next_months_since_last_restock").desc_nulls_first(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("row_seq").desc())
amount_by_store_id = df_orders.groupBy("store_id").agg(F.avg("amount").alias("avg_order_amount"))

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-03-28")))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        (F.datediff(F.current_date(), "last_restock") / 30).alias("months_since_last_restock"),
    )
)

metrics = (
    base
    .filter(F.col("quantity").between(161, 200))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "months_since_last_restock",
        F.lead("months_since_last_restock").over(w).alias("next_months_since_last_restock"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("row_seq"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "months_since_last_restock",
        "next_quantity",
        "row_num",
        F.col("avg_order_amount"),
    )
)
