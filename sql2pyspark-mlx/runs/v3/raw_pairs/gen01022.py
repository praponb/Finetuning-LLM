from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%ARO%") & (F.upper(F.trim("email")) == "GRACE.KIM@GMAIL.COM") & F.col("full_name").isNotNull())
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(F.countDistinct("salary").alias("unique_managers"))
)
