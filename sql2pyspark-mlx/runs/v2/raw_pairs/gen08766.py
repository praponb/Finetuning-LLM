from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([9, 11, 12]) & (F.length("store_name") >= 6) & ~F.col("region").isin(["North", "East", "West"]))
    .select("store_name", "sqft", "opened_date")
    .orderBy("sqft")
)
