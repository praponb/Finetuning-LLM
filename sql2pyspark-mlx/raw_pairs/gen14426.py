from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")
w2 = Window.partitionBy(F.month("opened_date"), "city").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.ntile(5).over(w).alias("sqft_bucket"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
