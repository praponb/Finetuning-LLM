from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Bangkok") & (F.month("opened_date").isin([2, 3, 12]) | (F.col("city") == "Tokyo")))
    .select("id", "sqft", "opened_date", "city")
    .orderBy("city", "id")
    .limit(5)
)
