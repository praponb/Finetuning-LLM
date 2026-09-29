from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 310)
    .select("supplier_id")
    .subtract(
        df_products
        .filter(F.col("launch_date") > "2023-09-18")
        .select("supplier_id")
    )
    .orderBy("supplier_id")
)
