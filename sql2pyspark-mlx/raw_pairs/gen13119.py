from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter((F.col("units") >= 35) & F.month("sale_date").isin([6, 9, 11]))
)

sale_year_summary = (
    src
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    sale_year_summary
    .select("sale_year", "latest_sale_date")
    .orderBy(F.col("sale_year").desc(), "latest_sale_date")
)
