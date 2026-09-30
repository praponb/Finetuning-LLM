from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Tokyo", "Chiang Mai"]) & (F.col("dept_name") == "Marketing") & F.col("dept_name").isin(["Marketing", "HR", "Legal"]))
    .select("id", "location")
)
