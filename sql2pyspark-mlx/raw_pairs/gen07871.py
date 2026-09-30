from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("brand")
w5 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("price", "id")

filtered = (
    df_products
    .filter((~F.col("brand").eqNullSafe(F.lit("Initech"))) & (F.col("launch_date") > F.lit("2023-09-01").cast("date")))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.concat("product_name", F.lit(" - "), "brand").alias("product_name_brand"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

scored = (
    with_latest
    .filter(F.col("stock_qty") >= 153)
    .select(
        "*",
        F.avg("stock_qty").over(w3).alias("stock_qty_avg_6_prior"),
        F.round((F.col("price") / F.sum("price").over(w4)), 4).alias("price_share"),
        F.last("cost", ignorenulls=True).over(w5).alias("last_known_cost"),
        F.lead("cost", 1).over(w6).alias("next_cost"),
    )
)

result = (
    scored
    .select(
        "id",
        "price",
        "cost",
        "tags",
        "supplier_id",
        "product_name_brand",
        "inventory_last_restock",
        "unit_price",
        "stock_qty_avg_6_prior",
    )
)
