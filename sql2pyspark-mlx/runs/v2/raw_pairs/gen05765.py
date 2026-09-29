from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") == 16)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.round((F.col("units") * 0.9), 2).alias("adjusted_units"),
        F.round((F.col("revenue") * 1.15), 2).alias("adjusted_revenue"),
    )
)
