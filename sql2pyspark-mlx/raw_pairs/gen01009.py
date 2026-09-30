from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

result = (
    valid
    .select("id", "product_id", "unit_price", "quantity_filled")
    .unpivot(
        ids=["id", "product_id"],
        values=["unit_price", "quantity_filled"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
