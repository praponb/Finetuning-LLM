from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([3, 4, 6]))
    .select("id", "cost", "supplier_id")
)
