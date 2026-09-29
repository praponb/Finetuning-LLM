from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("dept_name_length").desc_nulls_first(), "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("location") == "Tokyo"))
    .withColumn("dept_name_length", F.length("dept_name"))
)

kept = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([16, 27, 33])), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.lower(F.trim(F.col("c.email"))) == "jack@startup.io"), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select("id", "dept_name", "location", "dept_name_length", "rn")
    .orderBy("dept_name_length", "location")
)
