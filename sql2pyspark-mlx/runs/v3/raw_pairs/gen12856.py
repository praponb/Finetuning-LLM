from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Umbrella") & F.lower("product_name").like("%oga%"))
    .select("id", "launch_date", "product_name", "category")
    .orderBy("launch_date", "id")
    .limit(3)
)
