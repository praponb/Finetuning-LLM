from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy("last_restock_weekofyear", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("last_restock_weekofyear").desc_nulls_first(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("event_seq").asc_nulls_first())

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-04-05")))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
        F.weekofyear("last_restock").alias("last_restock_weekofyear"),
    )
)

metrics = (
    valid
    .filter(F.col("product_id") == 1)
    .select(
        "*",
        F.row_number().over(w_hist).alias("event_seq"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    deduped
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.cost"), F.col("p.price"))
)

latest = (
    joined
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "product_id",
        "running_quantity",
        "latest_rank",
        "cost",
        "price",
        "rank_in_group",
    )
    .orderBy(F.col("cost").desc_nulls_first())
)
