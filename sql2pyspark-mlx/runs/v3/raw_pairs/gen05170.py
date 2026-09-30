from pyspark.sql import functions as F

src = df_store_sales.filter((F.col("units") > 28) & (F.col("units") != 31))

store_id_summary = (
    src
    .groupBy(
        "store_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = (
    store_id_summary
    .select("store_id", "sale_cohort", "first_sale_date", "min_sale_date")
    .orderBy(F.col("min_sale_date").desc(), "store_id")
)
