from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "supplier_id",
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        F.when(F.col("price") >= 1160, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("stock_qty_per_stark"),
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("cost"))).alias("fashion_total_cost"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("cost"))).alias("electronics_total_cost"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
    .filter(F.col("corr_stock_qty_price") > 0)
    .orderBy(F.col("supplier_id").asc_nulls_first())
)
