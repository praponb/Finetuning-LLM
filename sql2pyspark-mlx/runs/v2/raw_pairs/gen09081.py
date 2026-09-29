from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Tokyo", "Chiang Mai", "Bangkok"])
    .agg(F.count("budget"))
)
