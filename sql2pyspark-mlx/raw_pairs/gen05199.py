from pyspark.sql import functions as F

by_dimension = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .groupingSets(
        [["country", "city"], ["country"], []],
        F.col("d.country").alias("country"),
        F.col("d.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.stock_qty")), 2).alias("total_stock_qty"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select("num_rows", "total_stock_qty", "grouping_level")
    .orderBy(F.col("grouping_level").desc_nulls_last(), "num_rows")
)
