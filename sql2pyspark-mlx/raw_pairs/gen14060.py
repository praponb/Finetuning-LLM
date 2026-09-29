from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").desc(), "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("dedup_rank", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .filter(F.col("line_total") > 50)
    .select(
        "id",
        "product_id",
        "unit_price",
        "line_total",
        "dedup_rank",
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.lead("unit_price", 2).over(w3).alias("next_unit_price"),
    )
)

remaining_keys = (
    enriched
    .select("id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([5, 16]))
        .select("id", "product_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("product_id", "next_unit_price", "line_total", "unit_price")
    .unpivot(
        ids=["product_id"],
        values=["next_unit_price", "line_total", "unit_price"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
