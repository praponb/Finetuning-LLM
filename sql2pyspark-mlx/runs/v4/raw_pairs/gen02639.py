from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("pos", "id", "pos")
w2 = Window.partitionBy("customer_id").orderBy(F.col("price").desc(), "id", "pos")
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("customer_id").orderBy("price_bucket", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        F.col("line.product_id"),
        F.col("line.price"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.lag("product_id", 2).over(w_part).alias("prev_product_id"),
        F.ntile(5).over(w2).alias("price_bucket"),
        F.sum("pos").over(w3).alias("customer_id_sum_pos"),
    )
)

with_windows = (
    enriched
    .withColumn("prev_product_id_share", F.round((F.col("prev_product_id") / F.sum("prev_product_id").over(w3)), 4))
    .withColumn("first_known_pos", F.first("pos", ignorenulls=True).over(w4))
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        "product_id",
        "price",
        "customer_id_sum_pos",
        "prev_product_id_share",
        F.col("max_account_balance"),
    )
    .orderBy(F.col("prev_product_id_share").desc())
)
