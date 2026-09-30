from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").asc_nulls_first(), "id")

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2024-02-07").cast("date"))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
    .withColumn("account_type_status", F.concat("account_type", F.lit("/"), "status"))
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
