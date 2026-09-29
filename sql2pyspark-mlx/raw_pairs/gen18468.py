from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("unique_orders") > 4)
    .orderBy(F.col("shipped_year").desc())
)
