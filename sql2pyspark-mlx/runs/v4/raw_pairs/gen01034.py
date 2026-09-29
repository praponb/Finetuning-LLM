from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(
        F.col("ss.product_id"),
        F.when(F.year(F.col("ss.sale_date")) < 2024, "before_2024").when(F.year(F.col("ss.sale_date")) == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("sale_cohort").desc())
)
