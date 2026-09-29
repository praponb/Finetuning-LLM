from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("quantity", "id")

enriched = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.lag("quantity", 1, 0).over(w_recent).alias("prev_quantity"),
        F.lag("quantity", 1).over(w_recent).alias("prev_quantity_2"),
    )
)

result = (
    enriched
    .groupBy(
        "product_id",
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("prev_quantity", 0.95).alias("prev_quantity_p95"),
        F.percentile_approx("prev_quantity_2", [0.25, 0.5, 0.75]).alias("prev_quantity_2_quartiles"),
    )
    .orderBy("avg_quantity")
)
