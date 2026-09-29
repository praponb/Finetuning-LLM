from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3666000)
    .select("id", "location")
    .intersect(
        df_departments
        .filter(F.col("budget") > 2584000)
        .select("id", "location")
    )
    .orderBy("id")
)
