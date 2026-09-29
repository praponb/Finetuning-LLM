from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        "status",
        F.row_number().over(w).alias("row_num"),
        F.lag("opened_date", 1).over(w2).alias("prev_opened_date"),
    )
    .orderBy(F.col("prev_opened_date").desc())
)
