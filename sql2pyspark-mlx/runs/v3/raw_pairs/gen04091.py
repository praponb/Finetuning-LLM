from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Globex", "Acme"]) & (F.col("cost") > 640) & F.col("stock_qty").between(270, 332))
    .select(
        "stock_qty",
        "launch_date",
        "tags",
        F.round("stock_qty", 0).alias("stock_qty_rounded"),
        F.date_add("launch_date", 86).alias("launch_date_plus_86d"),
    )
    .orderBy(F.col("stock_qty_rounded").desc())
)
