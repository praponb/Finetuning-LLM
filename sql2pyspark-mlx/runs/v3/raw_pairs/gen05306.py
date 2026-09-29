from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["London", "Bangkok", "New York"]))
    .select("opened_date")
    .subtract(df_stores.filter(F.year("opened_date") == 2024).select("opened_date"))
)
