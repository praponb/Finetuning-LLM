from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "location")
    .exceptAll(
        df_departments
        .filter(~F.col("location").isin(["London", "Berlin", "New York"]))
        .select("id", "location")
    )
)
