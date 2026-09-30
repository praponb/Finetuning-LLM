from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select("page")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").isNotNull())
        .select(F.col("supplier_name").alias("page"))
    )
)
