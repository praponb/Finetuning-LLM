from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([4, 5, 6]))
    .select("id", "city")
)
