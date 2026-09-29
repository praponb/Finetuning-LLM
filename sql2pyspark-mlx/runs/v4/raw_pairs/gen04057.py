from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter((F.col("e.hire_date") <= "2025-01-02") | F.col("d.budget").between(1206000, 2334000))
    .select(
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.email"),
        F.col("e.is_remote"),
        F.col("e.full_name"),
        F.when(F.col("d.dept_name").isin(["HR", "Sales"]), "Tier 1").otherwise("Other").alias("dept_name_group"),
    )
)
