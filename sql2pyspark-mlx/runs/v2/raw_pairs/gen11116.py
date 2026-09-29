from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Marketing", "Legal"]) | F.col("location").isin(["New York", "Chiang Mai"]))
    .select("dept_name", "location", "budget")
)
