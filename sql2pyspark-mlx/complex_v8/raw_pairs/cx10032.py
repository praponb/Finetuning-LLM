from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_unit_price = Window.partitionBy("operator").orderBy(F.col("unit_price").desc(), "id")

valid_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

metrics = (
    valid_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        (F.col("unit_price") - F.lag("unit_price").over(w_operator_by_unit_price)).alias("unit_price_change"),
        F.dense_rank().over(w_operator_by_unit_price).alias("unit_price_dense_rank"),
    )
)

first_rows = metrics.filter(F.col("unit_price_dense_rank") <= 3)

result = (
    first_rows
    .select("id", "warehouse_id", "unit_price_change")
    .orderBy("unit_price_change")
)
