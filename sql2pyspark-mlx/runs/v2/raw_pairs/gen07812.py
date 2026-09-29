from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") == 2023)
    .select("category")
    .distinct()
    .orderBy("category")
    .limit(1)
)
