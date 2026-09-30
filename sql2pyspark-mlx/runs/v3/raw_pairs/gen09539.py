from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(~F.col("location").isin(["Chiang Mai", "Berlin", "Bangkok"]) | F.col("dept_name").isin(["Marketing", "HR"])))
)
