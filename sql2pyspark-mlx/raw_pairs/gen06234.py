from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Umbrella", "Acme"]) & (F.col("brand") == "Initech"))
    .select(
        "id",
        "cost",
        "stock_qty",
        F.sort_array("tags").alias("sorted_tags"),
        F.add_months("launch_date", 6).alias("launch_date_plus_months"),
    )
    .orderBy("stock_qty", F.col("id").desc())
    .limit(20)
)
