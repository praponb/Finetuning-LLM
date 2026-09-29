from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.orderBy(F.col("budget").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_departments
    .filter(~F.col("location").isin(["Tokyo", "Chiang Mai"]))
    .select(
        "id",
        "location",
        "budget",
        F.round((F.col("budget") / F.sum("budget").over(w)), 3).alias("budget_share"),
        F.last("location").over(w2).alias("last_location"),
    )
)
