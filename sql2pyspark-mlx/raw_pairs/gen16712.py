from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location", "dept_name")

enriched = (
    df_departments
    .withColumn("first_known_budget", F.first("budget", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("location_sum_budget", F.sum("budget").over(w3))
)

result = (
    enriched
    .select(
        "id",
        "location",
        "budget",
        "row_seq",
        "location_sum_budget",
        F.when(F.col("location").rlike("^(London|New York)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("location_zone"),
    )
    .orderBy(F.col("location_zone").desc())
)
