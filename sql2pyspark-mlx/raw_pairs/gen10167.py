from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1300, 2800) & F.col("units").between(16, 18))
    .select(
        "id",
        "units",
        "store_id",
        F.quarter("sale_date").alias("sale_date_quarter"),
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
    )
)
