from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("unit_price_scaled")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "brand").orderBy("unit_price_rounded_filled", "id")
w4 = Window.partitionBy("brand", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isin([4, 10, 19, 34]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round("unit_price", 1).alias("unit_price_rounded"),
    )
)

cleaned = (
    valid
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_rounded",
        F.round((F.col("unit_price") * 0.9), 2).alias("unit_price_scaled"),
    )
)

base = (
    cleaned
    .filter(F.col("product_id").isNotNull() & F.col("unit_price_scaled").between(27, 32))
    .withColumn("unit_price_rounded_filled", F.coalesce("unit_price_rounded", F.lit(0)))
)

with_dims = (
    base
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.price"), F.col("p.brand"))
)

result = (
    with_dims
    .select(
        "id",
        "quantity",
        "unit_price",
        "unit_price_scaled",
        "unit_price_rounded_filled",
        "price",
        "brand",
        F.cume_dist().over(w).alias("unit_price_scaled_cume_dist"),
        F.last("unit_price_scaled", ignorenulls=True).over(w2).alias("last_known_unit_price_scaled"),
        F.lead("unit_price_rounded", 1).over(w3).alias("next_unit_price_rounded"),
        F.sum("unit_price_scaled").over(w4).alias("running_unit_price_scaled"),
    )
)
