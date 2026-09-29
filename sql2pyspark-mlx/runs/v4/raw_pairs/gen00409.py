from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w_hist = Window.orderBy("adjusted_amount", "id")
w3 = Window.partitionBy("product_id", "order_id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.amount"))
)

filtered = (
    enriched
    .filter(F.col("amount") >= 810)
    .select(
        "id",
        "order_id",
        "product_id",
        "amount",
        F.round((F.col("amount") * 1.1), 2).alias("adjusted_amount"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

cleaned_order_items = (
    latest
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "adjusted_amount",
        F.col("adjusted_amount").cast("int").alias("adjusted_amount_int"),
    )
)

result = (
    cleaned_order_items
    .withColumn("next_adjusted_amount", F.lead("adjusted_amount", 1).over(w_hist))
    .withColumn("product_id_min_adjusted_amount", F.min("adjusted_amount").over(w3))
)
