from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy("stock_qty", "id")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")
w_ordered = Window.partitionBy("city", "brand").orderBy("cost_bucket", "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

base = (
    df_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Stark")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.size("tags").alias("tags_count"),
    )
)

metrics = (
    base
    .select(
        "id",
        "product_name",
        "brand",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.ntile(4).over(w).alias("stock_qty_bucket"),
        F.ntile(3).over(w2).alias("cost_bucket"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.city"))
)

scored = (
    joined
    .filter(F.length("product_name") > 6)
    .select(
        "id",
        "brand",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "stock_qty_bucket",
        "city",
        F.lag("cost_bucket").over(w_ordered).alias("prev_cost_bucket"),
        F.lead("stock_qty_bucket").over(w_ordered).alias("next_stock_qty_bucket"),
        F.lag("stock_qty_bucket").over(w_ordered).alias("prev_stock_qty_bucket"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

result = (
    scored
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "launch_date",
        "supplier_id",
        "stock_qty_bucket",
        "city",
        "prev_cost_bucket",
        "next_stock_qty_bucket",
        F.col("min_product_cost"),
    )
    .orderBy(F.col("supplier_id").desc())
)
