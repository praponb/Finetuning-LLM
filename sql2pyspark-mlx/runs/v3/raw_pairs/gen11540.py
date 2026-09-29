from pyspark.sql import functions as F

base = df_products.filter((F.size("tags") > 2) & F.col("supplier_id").isNull())

summary = (
    base
    .groupBy(
        F.quarter("launch_date").alias("launch_quarter"),
        F.year("launch_date").alias("launch_year"),
    )
    .agg(F.sum("price").alias("total_price"))
)

result = (
    summary
    .select("launch_quarter", "launch_year", "total_price")
    .orderBy(F.col("launch_year").desc(), "launch_quarter")
)
