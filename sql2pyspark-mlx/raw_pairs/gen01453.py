from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.upper(F.trim("email")) == "FRANK@COMPANY.CO") & ((F.col("hire_date") > "2024-02-24") | F.col("level").isNull()))
    .groupBy("job_title")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .orderBy("job_title")
)
