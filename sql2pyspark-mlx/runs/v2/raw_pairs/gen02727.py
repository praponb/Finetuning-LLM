from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3]))
    .select(
        "id",
        "units",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
        F.quarter("sale_date").alias("sale_date_quarter"),
    )
)
