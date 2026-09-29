from pyspark.sql import functions as F

kept_keys = (
    df_products
    .select("supplier_id")
    .subtract(
        df_products
        .filter(F.size("tags") > 1)
        .select("supplier_id")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.max(F.when(F.col("category") == "Fashion", F.col("cost"))).alias("fashion_max_cost"),
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("category") == "Books", F.col("stock_qty"))).alias("books_total_stock_qty"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
    )
    .filter(F.col("row_count") < 1000)
)
