from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "revenue",
        "store_id",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
    )
)
