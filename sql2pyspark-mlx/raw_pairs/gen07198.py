from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("last_restock_quarter").desc_nulls_first())
w2 = Window.partitionBy(F.date_trunc("month", "last_restock"), "product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w4 = Window.partitionBy("category")
w5 = Window.orderBy(F.col("cost_share").desc_nulls_last())

kept_keys = (
    df_inventory
    .select("store_id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock").between("2023-08-10", "2025-10-01"))
        .select("store_id", "product_id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"), F.col("p.category"), F.col("p.brand"))
)

valid = (
    joined
    .filter((F.col("last_restock") >= "2023-08-08") & F.col("category").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "cost",
        "category",
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
)

with_windows = (
    valid
    .withColumn("last_restock_quarter_dense_rank", F.dense_rank().over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
)

with_windows_inventory = (
    with_windows
    .select(
        "quantity",
        "cost",
        "category",
        "last_restock_quarter",
        F.rank().over(w3).alias("quantity_rank"),
        F.max("cost").over(w4).alias("category_max_cost"),
        F.percent_rank().over(w5).alias("cost_share_percent_rank"),
    )
)

result = (
    with_windows_inventory
    .select("category", "cost", "cost_share_percent_rank")
    .unpivot(
        ids=["category"],
        values=["cost", "cost_share_percent_rank"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
