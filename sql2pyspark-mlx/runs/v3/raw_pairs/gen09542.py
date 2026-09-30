from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("cost").alias("unique_categorys"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("stock_qty")).otherwise(0)).alias("umbrella_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("ss.unique_categorys"),
        F.col("ss.umbrella_stock_qty"),
    )
    .orderBy(F.col("unique_categorys").desc())
)
