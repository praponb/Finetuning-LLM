from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w_hist = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("signup_date"), "city").orderBy("age", "id")

metrics = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .select(
        "*",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.first("city", ignorenulls=True).over(w_hist).alias("first_known_city"),
        F.ntile(5).over(w3).alias("age_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "age",
        "credit_limit_share",
        F.when(F.col("name").rlike("^(David Lee |Alice Wong)$"), "GROUP_B").when(F.col("name").rlike("^(bob smith|  Carol Diaz)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("name_zone"),
    )
    .orderBy(F.col("city").desc(), "country")
)
