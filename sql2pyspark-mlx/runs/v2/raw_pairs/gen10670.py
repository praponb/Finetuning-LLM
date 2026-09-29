from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("full_name").like("%MILLE%") & ~F.col("job_title").isin(["Manager", "Engineer", "Director"]) & (F.col("salary") > 125000))
)
