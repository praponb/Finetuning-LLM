from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "COFFEE MAKER")
    .select("id", "launch_date", "cost")
    .orderBy("cost", "launch_date")
)
