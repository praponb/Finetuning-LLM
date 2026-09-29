from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")
w_recent = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.category").isin(["Fashion", "Electronics", "Home"]))
    .select("a.*", F.col("p.cost"))
)

deduped = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "product_id", "quantity", "cost")
)

joined = (
    deduped
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.quantity",
        "src.cost",
        F.col("o.order_date"),
        F.col("o.amount"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "quantity",
        "cost",
        "amount",
        F.row_number().over(w2).alias("row_seq"),
        F.last("quantity", ignorenulls=True).over(w_recent).alias("last_known_quantity"),
    )
)

filtered = (
    enriched
    .filter(F.col("quantity") < 6)
    .select(
        "id",
        "cost",
        "amount",
        "row_seq",
        "last_known_quantity",
        (F.col("last_known_quantity") + F.col("quantity")).alias("last_known_quantity_plus_quantity"),
        (F.col("amount") + F.col("cost")).alias("amount_plus_cost"),
    )
)

result = (
    filtered
    .select(
        "id",
        "cost",
        "amount",
        "last_known_quantity",
        "last_known_quantity_plus_quantity",
    )
)
