from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Initech", "Globex"]))
    .select("cost", "launch_date")
    .orderBy("launch_date")
)
