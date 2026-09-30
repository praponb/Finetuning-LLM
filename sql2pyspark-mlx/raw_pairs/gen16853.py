from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-06-01", "2024-05-02"))
    .select(
        "id",
        "sqft",
        "region",
        "opened_date",
        F.rank().over(w).alias("sqft_rank"),
        F.last("sqft").over(w2).alias("last_sqft"),
    )
)
