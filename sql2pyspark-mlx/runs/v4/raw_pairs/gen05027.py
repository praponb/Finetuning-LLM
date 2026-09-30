from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_customers
    .select("id", "signup_date", "city", F.last("city").over(w).alias("last_city"))
)
