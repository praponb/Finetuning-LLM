from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

joined = (
    df_inventory
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.category") == "Books")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.quantity",
        "e.last_restock",
        F.col("b.price"),
    )
)

base = (
    joined
    .filter((F.col("last_restock") >= F.lit("2023-09-12").cast("date")) & F.month("last_restock").isin([7, 9, 11]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "price",
        F.col("price").cast("int").alias("price_int"),
    )
)

scored = (
    base
    .select(
        "id",
        "product_id",
        "price_int",
        F.ntile(3).over(w).alias("quantity_bucket"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.last("price", ignorenulls=True).over(w3).alias("last_known_price"),
        F.first("quantity", ignorenulls=True).over(w4).alias("first_known_quantity_2"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.price_int",
        "e.first_known_quantity",
        "e.last_known_price",
        F.col("p.stock_qty"),
    )
)

result = (
    enriched
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "first_known_quantity",
        "last_known_price",
        "stock_qty",
        F.col("sum_store_sale_revenue"),
    )
    .orderBy(F.col("sum_store_sale_revenue").desc(), "product_id")
)
