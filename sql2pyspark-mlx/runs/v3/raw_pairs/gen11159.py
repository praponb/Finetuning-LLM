from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "units",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
        F.date_add("sale_date", 74).alias("sale_date_plus_74d"),
    )
)
