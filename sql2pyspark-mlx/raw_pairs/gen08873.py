from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("category").orderBy("stock_qty", "id")
w2 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("stock_qty").alias("avg_product_stock_qty"))

kept = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.launch_date")), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.units") > 14), "left_anti")
)

metrics = (
    eligible
    .select(
        "*",
        F.lag("cost").over(w_part).alias("prev_cost"),
        F.lag("stock_qty").over(w_part).alias("prev_stock_qty"),
        F.row_number().over(w_part).alias("row_seq"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)

kept_keys = (
    metrics
    .select("category", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("cost") > 520)
        .select("category", "supplier_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "supplier_id",
        "prev_cost",
        F.col("avg_product_stock_qty"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
