from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") > 380)
    .select("launch_date", "category")
    .subtract(
        df_products
        .filter(F.year("launch_date") >= 2024)
        .select("launch_date", "category")
    )
)
